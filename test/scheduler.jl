import Dagger.Sch: SchedulerOptions, ThunkOptions, SchedulerHaltedException, ComputeState

@everywhere begin
using Dagger
function inc(x)
    x+1
end
function checkwid(x...)
    @assert myid() == 1
    return 1
end
function checktid(x...)
    @assert Threads.threadid() != 1 || Threads.nthreads() == 1
    return 1
end
function dynamic_exec(h, x)
    Dagger.Sch.exec!(h) do state
        if state isa ComputeState
            return 1
        else
            return 0
        end
    end
end
function dynamic_exec_err(h, x)
    Dagger.Sch.exec!(h) do state
        error("An error")
    end
end
function dynamic_halt(h, x)
    Dagger.Sch.halt!(h)
    return x
end
function dynamic_get_dag(h, x...)
    ids = Dagger.Sch.get_dag_ids(h)
    return (h.thunk_id, ids)
end
end

@testset "Scheduler" begin
    @testset "Scheduler options: single worker" begin
        options = SchedulerOptions(;single=1)
        a = delayed(checkwid)(1)
        b = delayed(checkwid)(2)
        c = delayed(checkwid)(a,b)

        @test collect(Context(), c; options=options) == 1
    end
    @testset "Thunk options: single worker" begin
        options = ThunkOptions(;single=1)
        a = delayed(checkwid; options=options)(1)

        @test collect(Context(), a) == 1
    end
    @static if VERSION >= v"1.3.0-DEV.573"
        if Threads.nthreads() == 1
            @warn "Threading tests running in serial"
        end
        @testset "Scheduler options: threads" begin
            options = SchedulerOptions(;proctypes=[Dagger.ThreadProc])
            a = delayed(checktid)(1)
            b = delayed(checktid)(2)
            c = delayed(checktid)(a,b)

            @test collect(Context(), c; options=options) == 1
        end
        @testset "Thunk options: threads" begin
            options = ThunkOptions(;proctypes=[Dagger.ThreadProc])
            a = delayed(checktid; options=options)(1)

            @test collect(Context(), a) == 1
        end
    end

    @everywhere Dagger.add_callback!(proc->FakeProc())
    @testset "Thunk options: proctypes" begin
        @test Dagger.iscompatible_arg(FakeProc(), nothing, 1) == true
        @test Dagger.iscompatible_arg(FakeProc(), nothing, FakeVal(1)) == true
        @test Dagger.iscompatible_arg(FakeProc(), nothing, 1.0) == false
        @test Dagger.default_enabled(Dagger.ThreadProc(1,1)) == true
        @test Dagger.default_enabled(FakeProc()) == false

        opts = Dagger.Sch.ThunkOptions(;proctypes=[Dagger.ThreadProc])
        as = [delayed(identity; options=opts)(i) for i in 1:5]
        opts = Dagger.Sch.ThunkOptions(;proctypes=[FakeProc])
        b = delayed(fakesum; options=opts)(as...)

        @test collect(Context(), b) == 57
    end
    @everywhere (pop!(Dagger.PROCESSOR_CALLBACKS); empty!(Dagger.OSPROC_CACHE))

end

@testset "Dynamic Thunks" begin
    @testset "Exec" begin
        a = delayed(dynamic_exec; dynamic=true)(2)
        @test collect(Context(), a) == 1
    end
    @testset "Exec Error" begin
        a = delayed(dynamic_exec_err; dynamic=true)(1)
        @test_throws RemoteException collect(Context(), a)
    end
    @testset "Halt" begin
        a = delayed(dynamic_halt; dynamic=true)(1)
        @test_throws SchedulerHaltedException collect(Context(), a)
    end
    @testset "DAG querying" begin
        a = delayed(identity)(1)
        b = delayed(x->x+2)(a)
        c = delayed(x->x-1)(a)
        d = delayed(dynamic_get_dag; dynamic=true)(b, c)
        (d_id, ids) = collect(Context(), d)
        @test ids isa Dict
        @test length(keys(ids)) == 4
        @test haskey(ids, d_id)
        d_deps = ids[d_id]
        @test length(d_deps) == 0
        a_id = d_id - 3 # relies on thunk ID monotonicity
        a_deps = ids[a_id]
        @test length(a_deps) == 2
        i1, i2 =  pop!(a_deps), pop!(a_deps)
        @test haskey(ids, i1)
        @test haskey(ids, i2)
        @test ids[pop!(ids[i1])] == ids[pop!(ids[i2])]
    end
end
