defmodule Zookeeper.Party do
  use GenServer
  require Logger

  @node_name "__party__"
  @moduledoc """
  A Zookeeper pool of party members.
  """

  #TODO: shallow party
  ## Client

  @doc """
  Join a party on the given path
  """
  def start(client, path, data) do
    GenServer.start(__MODULE__, {client, path, data})
  end

  @doc """
  Join a party on the given path
  """
  def start(client, path) do
    GenServer.start(__MODULE__, {client, path, <<>>})
  end

  @doc """
  Join a party on the given path
  """
  def start_link(client, path, data) do
    GenServer.start_link(__MODULE__, {client, path, data})
  end

  def start_link(client, path) do
    GenServer.start_link(__MODULE__, {client, path, <<>>})
  end

  @doc """
  Get current list of party members.
  """
  def members(pid) do
    GenServer.call(pid, :members)
  end

  def prefix(pid) do
    GenServer.call(pid, :prefix)
  end

  @doc """
  Leave the party
  """
  def leave(pid) do
    GenServer.call(pid, :leave)
  end

  ## Server

  def init({client, path, data}) do
    prefix = "#{UUID.uuid4(:hex)}#{@node_name}"
    state = %{
      client: client,
      path: path,
      prefix: prefix,
      data: data,
    }
    do_join(state)
  end

  def handle_call(:members, _from, state) do
    {:ok, members} = get_members(state)
    {:reply, members, state}
  end

  def handle_call(:leave, _from, state) do
    {:ok, state} = do_leave(state)
    {:stop, :normal, :ok, state}
  end

  def handle_call(:prefix, _from, state) do
    {:reply, state.prefix, state}
  end

  def handle_info({Zookeeper.Client, _path, :children}, state) do
    get_members(state)
  end

  def handle_info({:DOWN, _ref, :process, pid, reason}, %{client: pid}=state) do
    {:stop, reason, state}
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  ## Private

  def do_join(state) do
    ensure_node(state)
  end

  def do_leave(%{client: zk, path: path, prefix: prefix}=state) do
    :ok = Zookeeper.Client.delete(zk, "#{path}/#{prefix}")
    {:ok, state}
  end

  def get_members(state) do
    case Zookeeper.Client.get_children(state.client, state.path) do
      {:ok, members} -> {:ok, members}
      {:error, :no_node} ->
        case Zookeeper.Client.exists(state.client, state.path, self()) do
          # TODO: non-hammering retry logic. ditto Zookeeper.children_watch
          {:ok, _stat} -> get_members(state)
          {:error, :no_node} -> {:ok, []}
          {:error, reason} -> {:error, reason}
        end
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_node(%{client: zk, path: path, prefix: prefix, data: data}=state) do
    case Zookeeper.Client.create(zk, "#{path}/#{prefix}", data, makepath: true, create_mode: :ephemeral) do
      {:ok, _created_path} -> {:ok, state}
      {:node_exists, _created_path} -> {:ok, state}
    end
  end
end
