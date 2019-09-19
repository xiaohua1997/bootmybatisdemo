package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.source;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.Block;

public class ExpressionTable extends SourceTable implements Expression{
    private String _expression;
    private Block _blockNode;

    @Override
    public String get_expression() {
        return this._expression;
    }

    @Override
    public void set_expression(String expression) {
        this._expression = expression;
    }

    @Override
    public Block get_blockNode() {
        return this._blockNode;
    }

    @Override
    public void set_blockNode(Block blockNode) {
        this._blockNode = blockNode;
    }
}
