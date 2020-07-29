"A handle to the scheduler, used by dynamic thunks."
struct SchedulerHandle
    thunk_id::Int
    out_chan::RemoteChannel
    inp_chan::RemoteChannel
end

"Thrown when the scheduler halts before finishing processing the DAG."
struct SchedulerHaltedException <: Exception end

## Worker-side methods for dynamic communication

"Sends arbitrary data to the scheduler."
send!(h::SchedulerHandle, cmd, data) = put!(h.out_chan, (h.thunk_id, cmd, data))

"Receives the next message from the scheduler."
recv!(h::SchedulerHandle) = take!(h.inp_chan)

"Executes an arbitrary function within the scheduler, returning the result."
function exec!(f, h::SchedulerHandle)
    send!(h, :exec, f)
    failed, res = recv!(h)
    if failed
        throw(res)
    end
    res
end

"Commands the scheduler to halt execution immediately."
function halt!(h::SchedulerHandle)
    send!(h, :halt, nothing)
    sleep(1)
end

"Returns all Thunks IDs as a Dict, mapping a Thunk to its downstream dependents."
get_dag_ids(h::SchedulerHandle) = (send!(h, :get_dag_ids, nothing); recv!(h))

struct Arg
    x
end
"Adds a new Thunk to the DAG."
function add_thunk!(f, h::SchedulerHandle, args...; kwargs...)
    send!(h, :add_thunk, (f, args, kwargs))
    recv!(h)
end

"Sets the Thunk to return from the DAG."
function set_return!(h::SchedulerHandle, id)
    send!(h, :set_return, id)
end

## Scheduler-side methods for dynamic communication

# Fallback method
function process_dynamic!(state, task, tid, cmd, data)
    @warn "Received invalid dynamic command $cmd from thunk $tid: $data"
    Base.throwto(task, SchedulerHaltedException())
end

function process_dynamic!(state, task, tid, cmd::Val{:exec}, f)
    try
        res = lock(state.lock) do
            Base.invokelatest(f, state)
        end
        return (false, res)
    catch err
        return (true, err)
    end
end

function process_dynamic!(state, task, tid, cmd::Val{:halt}, _)
    state.halt[] = true
    Base.throwto(task, SchedulerHaltedException())
end
function safepoint(state, chan)
    if state.halt[]
        # Force dynamic thunks and listeners to terminate
        for (inp_chan,out_chan) in values(state.worker_chans)
            close(inp_chan)
            close(out_chan)
        end
        # Throw out of scheduler
        throw(SchedulerHaltedException())
    end
end

function process_dynamic!(state, task, tid, cmd::Val{:get_dag_ids}, _)
    deps = Dict{Int,Set{Int}}()
    for (key,val) in state.dependents
        deps[key.id] = Set(map(t->t.id, collect(val)))
    end
    deps
end

function process_dynamic!(state, task, tid, cmd::Val{:add_thunk}, (f, args, kwargs))
    _args = map(arg->arg isa Arg ? arg.x : state.thunk_dict[arg], args)
    thunk = Thunk(f, _args...; kwargs...)
    lock(state.lock) do
        state.thunk_dict[thunk.id] = thunk
        state.dependents[thunk] = Set{Thunk}()
        reschedule_inputs!(state, thunk)
        if isempty(state.waiting[thunk])
            push!(state.ready, thunk)
        end
    end
    return thunk.id
end

function process_dynamic!(state, task, tid, cmd::Val{:set_return}, id)
    lock(state.lock) do
        state.return_thunk[] = state.thunk_dict[id]
    end
    nothing
end

"Processes dynamic messages from worker-executing thunks."
function dynamic_listener!(state)
    task = current_task()
    for tid in keys(state.worker_chans)
        inp_chan, out_chan = state.worker_chans[tid]
        @async begin
            while isopen(inp_chan)
                try
                    tid, cmd, data = take!(inp_chan)
                    res = process_dynamic!(state, task, tid, Val(cmd), data)
                    res !== nothing && put!(out_chan, res)
                catch err
                    err = unwrap_nested_exception(err)
                    err isa SchedulerHaltedException && break
                    err isa ProcessExitedException && break
                    err isa InvalidStateException && err.state == :closed && break
                    @error exception=(err,catch_backtrace())
                    break
                end
            end
        end
    end
end
