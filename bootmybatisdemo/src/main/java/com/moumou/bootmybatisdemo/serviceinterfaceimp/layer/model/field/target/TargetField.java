package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.target;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.Direct;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.Field;

public class TargetField extends Field implements Direct {
    private String _fieldName;

    @Override
    public String get_fieldName() {
        return _fieldName;
    }

    @Override
    public void set_fieldName(String fieldName) {
        this._fieldName = fieldName;
    }
}
