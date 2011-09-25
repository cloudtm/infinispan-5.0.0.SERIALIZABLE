package org.infinispan.mvcc;

import org.infinispan.remoting.transport.Address;

import java.util.List;

/**
 * @author pedro
 *         Date: 08-08-2011
 */
public class ReplGroup {
    private int id;
    private List<Address> members;

    public ReplGroup(int id, List<Address> addrs) {
        this.id = id;
        this.members = addrs;
    }

    public int getId() {
        return id;
    }

    public List<Address> getMembers() {
        return members;
    }

    @Override
    public String toString() {
        return "ReplicationGroup{id=" + id + ",members=" + members + "}";
    }
}
