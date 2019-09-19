package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.source;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.Block;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.condition.JoinCondition;

public class JoinExpressionTable extends SourceTable implements Expression, Join{
    private JoinType _joinType;
    private JoinCondition _condition;
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

    @Override
    public void set_joinType(JoinType joinType) {
        this._joinType = joinType;
    }

    @Override
    public JoinType get_joinType() {
        return this._joinType;
    }

    @Override
    public void set_Condition(JoinCondition condition) {
        this._condition = condition;
    }

    @Override
    public JoinCondition get_Condition() {
        return this._condition;
    }
}

