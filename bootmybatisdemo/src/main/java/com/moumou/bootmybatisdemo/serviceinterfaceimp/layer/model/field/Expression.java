package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.source.DirectField;

import java.util.List;

public interface Expression {
    public void set_expression(String expression);
    public String get_expression();

    public void set_directFields(List<DirectField> directFields);
    public List<DirectField> get_directFields();
}
