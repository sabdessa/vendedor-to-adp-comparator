package com.kramp;

public class EnvConfig {
    private final String adpUser;
    private final String vendedorUser;
    private final String project;


    public EnvConfig(String adpUser, String vendedorUser, String project) {
        this.adpUser = adpUser;
        this.vendedorUser = vendedorUser;
        this.project = project;
    }

    public String getProject() {
        return project;
    }

    public String getAdpUser() {
        return adpUser;
    }

    public String getVendedorUser() {
        return vendedorUser;
    }
}
