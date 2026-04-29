package com.aazim.kvstore.model;

public class MemberInfo {

    private String address;
    private NodeState state;
    private long heartbeat;
    // lastSeen is local-only monotonic time (System.nanoTime() in ms via TimeUnit).
    // Never propagated in gossip — the receiver always resets it to its own monotonicMs().
    // Using nanoTime avoids NTP/wall-clock jumps corrupting failure detection timeouts.
    private long lastSeen;

    public MemberInfo() {}

    public MemberInfo(String address, NodeState state, long heartbeat, long lastSeen) {
        this.address = address;
        this.state = state;
        this.heartbeat = heartbeat;
        this.lastSeen = lastSeen;
    }

    public String getAddress()          { return address; }
    public NodeState getState()         { return state; }
    public long getHeartbeat()          { return heartbeat; }
    public long getLastSeen()           { return lastSeen; }

    public void setAddress(String address)      { this.address = address; }
    public void setState(NodeState state)       { this.state = state; }
    public void setHeartbeat(long heartbeat)    { this.heartbeat = heartbeat; }
    public void setLastSeen(long lastSeen)      { this.lastSeen = lastSeen; }
}
