package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.condition;

public abstract class Condition {
    private String _expression;
    private String _conditionDetails;

    public String get_expression() {
        return _expression;
    }

    public void set_expression(String expression) {
        this._expression = expression;
    }

    public String get_conditionDetails() {
        return _conditionDetails;
    }

    public void set_conditionDetails(String conditionDetails) {
        this._conditionDetails = conditionDetails;
    }
}
