package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.source;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.Block;

public interface Expression {
    public String get_expression();
    public void set_expression(String expression);

    public Block get_blockNode();
    public void set_blockNode(Block blockNode);
}
