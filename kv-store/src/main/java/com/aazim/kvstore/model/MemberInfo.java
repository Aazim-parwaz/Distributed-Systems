package com.aazim.kvstore.model;

public class MemberInfo {

    private String address;
    private NodeState state;
    private long heartbeat;
    // Incremented each time the node restarts. Merge key is (incarnation, heartbeat):
    // a higher incarnation always wins regardless of heartbeat, so a restarted node
    // (heartbeat reset to 0, incarnation bumped) can override its own DEAD entry.
    private long incarnation;
    // lastSeen is local-only monotonic time (System.nanoTime() in ms via TimeUnit).
    // Never propagated in gossip — the receiver always resets it to its own monotonicMs().
    // Using nanoTime avoids NTP/wall-clock jumps corrupting failure detection timeouts.
    private long lastSeen;

    public MemberInfo() {}

    public MemberInfo(String address, NodeState state, long heartbeat, long lastSeen, long incarnation) {
        this.address = address;
        this.state = state;
        this.heartbeat = heartbeat;
        this.lastSeen = lastSeen;
        this.incarnation = incarnation;
    }

    public String getAddress()          { return address; }
    public NodeState getState()         { return state; }
    public long getHeartbeat()          { return heartbeat; }
    public long getLastSeen()           { return lastSeen; }
    public long getIncarnation()        { return incarnation; }

    public void setAddress(String address)      { this.address = address; }
    public void setState(NodeState state)       { this.state = state; }
    public void setHeartbeat(long heartbeat)    { this.heartbeat = heartbeat; }
    public void setLastSeen(long lastSeen)      { this.lastSeen = lastSeen; }
    public void setIncarnation(long incarnation){ this.incarnation = incarnation; }
}
