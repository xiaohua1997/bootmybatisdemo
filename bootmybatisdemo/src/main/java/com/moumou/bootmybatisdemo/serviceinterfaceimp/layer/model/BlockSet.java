package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model;

import java.util.List;

public class BlockSet {
    private List<Block> _blocks;

    public String get_name() {
        return _name;
    }

    public void set_name(String name) {
        this._name = name;
    }

    private String _name;

    public List<Block> get_blocks() {
        return _blocks;
    }

    public void set_blocks(List<Block> blocks) {
        this._blocks = blocks;
    }
}
