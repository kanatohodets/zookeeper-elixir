defmodule Zookeeper.PartyTest do
  use ExUnit.Case

  alias Zookeeper.Client, as: ZK
  alias Zookeeper.Party, as: P
  @pool_path "exunit_party"

  setup_all do
    {:ok, pid} = ZK.start
    pid |> cleanup
    {:ok, zk_pid: pid}
  end

  setup %{zk_pid: zk_pid}=context do
    on_exit context, fn -> cleanup(zk_pid) end
    :ok
  end

  test "party_join", %{zk_pid: zk_pid} do
    member_pid = spawn_member(zk_pid)
    prefix = Zookeeper.Party.prefix(member_pid)
    [fetched_prefix] = Zookeeper.Party.members(member_pid)
    assert prefix === fetched_prefix
  end

  test "party_leave", %{zk_pid: zk_pid} do
    member_pids = for _ <- 0..1, do: spawn_member(zk_pid)
    a_member_pid = List.first(member_pids)
    b_member_pid = List.last(member_pids)

    party_ids = Zookeeper.Party.members(a_member_pid)
    assert 2 = Enum.count(party_ids)
    Zookeeper.Party.leave(a_member_pid)
    assert 1 = b_member_pid |> Zookeeper.Party.members |> Enum.count
  end

  test "party_leave_join", %{zk_pid: zk_pid} do
    member_pids = for _ <- 0..2, do: spawn_member(zk_pid)
    member_ids = for pid <- member_pids, do: Zookeeper.Party.prefix(pid)

    a_member = List.first(member_pids)
    a_prefix = Zookeeper.Party.prefix(a_member)
    b_member = List.last(member_pids)

    fetched_members = member_pids
                      |> List.last
                      |> Zookeeper.Party.members

    fetched_set = Enum.into(fetched_members, HashSet.new)
    starting_set = Enum.into(member_ids, HashSet.new)


    assert HashSet.equal?(fetched_set, starting_set)

    Zookeeper.Party.leave(a_member)

    new_member_set = Zookeeper.Party.members(b_member) |> Enum.into(HashSet.new)
    member_set_without_a = HashSet.delete(fetched_set, a_prefix)

    assert HashSet.equal?(new_member_set, member_set_without_a)
  end

  defp cleanup(zk_pid) do
    zk_pid |> ZK.delete(@pool_path, -1, True)
  end

  defp spawn_member(zk_pid) do
    {:ok, pid} = P.start(zk_pid, @pool_path)
    pid
  end
end
