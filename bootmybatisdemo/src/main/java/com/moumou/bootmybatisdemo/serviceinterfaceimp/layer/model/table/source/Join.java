package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.source;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.condition.JoinCondition;

public interface Join {
    public void set_joinType(JoinType joinType);
    public JoinType get_joinType();

    public void set_Condition(JoinCondition condition);
    public JoinCondition get_Condition();
}
