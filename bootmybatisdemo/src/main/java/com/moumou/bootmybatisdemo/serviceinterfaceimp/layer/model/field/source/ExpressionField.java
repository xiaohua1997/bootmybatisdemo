package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.source;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.Expression;

import java.util.List;

public class ExpressionField extends SourceField implements Expression {
    private String _expression;
    private List<DirectField> _directFields;

    @Override
    public void set_expression(String expression) {
        this._expression = expression;
    }

    @Override
    public String get_expression() {
        return this._expression;
    }

    @Override
    public void set_directFields(List<DirectField> directFields) {
        this._directFields = directFields;
    }

    @Override
    public List<DirectField> get_directFields() {
        return this._directFields;
    }
}
