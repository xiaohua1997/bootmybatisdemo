package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.source;

public enum JoinType {
    INNER_JOIN,
    LEFT_OUTER_JOIN,
    RIGHT_OUTER_JOIN,
    FULL_OUTER_JOIN,
    CROSS_JOIN
    ;

    public static JoinType parseJoinType(String joinType){
        if(null != joinType) {
            joinType = joinType.trim().toLowerCase();
        }

        switch (joinType) {
            case "inner join":
                return JoinType.INNER_JOIN;
            case "left outer join":
            case "left join":
                return JoinType.LEFT_OUTER_JOIN;
            case "right outer join":
            case "right join":
                return JoinType.RIGHT_OUTER_JOIN;
            case "full outer join":
            case "full join":
                return JoinType.FULL_OUTER_JOIN;
            case "cross join":
                return JoinType.CROSS_JOIN;
            default:
                return null;
        }
    }
}
