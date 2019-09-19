package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.condition.HavingCondition;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.condition.WhereCondition;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.source.Join;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.source.SourceTable;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.target.TargetTable;

import java.util.List;

public class Block {
    private TargetTable _targetTable;
    private SelectFields _select;
    private SourceTable _sourceTable;
    private List<Join> _joins;
    private WhereCondition _whereCondition;
    private GroupByFields _groupBy;
    private HavingCondition _havingCondition;

    public SelectFields get_select() {
        return _select;
    }

    public void set_select(SelectFields select) {
        this._select = select;
    }

    public SourceTable get_sourceTable() {
        return _sourceTable;
    }

    public void set_sourceTable(SourceTable sourceTable) {
        this._sourceTable = sourceTable;
    }

    public List<Join> get_joins() {
        return _joins;
    }

    public void set_joins(List<Join> joins) {
        this._joins = joins;
    }

    public WhereCondition get_whereCondition() {
        return _whereCondition;
    }

    public void set_whereCondition(WhereCondition whereCondition) {
        this._whereCondition = whereCondition;
    }

    public GroupByFields get_groupBy() {
        return _groupBy;
    }

    public void set_groupBy(GroupByFields groupBy) {
        this._groupBy = groupBy;
    }

    public HavingCondition get_havingCondition() {
        return _havingCondition;
    }

    public void set_havingCondition(HavingCondition havingCondition) {
        this._havingCondition = havingCondition;
    }

    public TargetTable get_targetTable() {
        return _targetTable;
    }

    public void set_targetTable(TargetTable targetTable) {
        this._targetTable = targetTable;
    }
}
