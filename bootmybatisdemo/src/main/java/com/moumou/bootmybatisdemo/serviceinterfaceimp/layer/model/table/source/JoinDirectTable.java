package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.source;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.condition.JoinCondition;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.Direct;

public class JoinDirectTable extends SourceTable implements Direct, Join{
    private String _schema;
    private String _tableName;
    private JoinType _joinType;
    private JoinCondition _condition;

    @Override
    public String get_schema() {
        return this._schema;
    }

    @Override
    public void set_schema(String schema) {
        this._schema = schema;
    }

    @Override
    public String get_tableName() {
        return this._tableName;
    }

    @Override
    public void set_tableName(String tableName) {
        this._tableName = tableName;
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
