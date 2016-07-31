package org.hcjf.main;

import java.util.Date;
import java.util.UUID;

/**
 * @author javaito
 * @mail javaito@gmail.com
 */
public class Auto {

    private UUID id;
    private String name;
    private Date lastUpdate;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(Date lastUpdate) {
        this.lastUpdate = lastUpdate;
    }
}
