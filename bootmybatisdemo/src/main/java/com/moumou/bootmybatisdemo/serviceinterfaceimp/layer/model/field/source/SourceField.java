package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.source;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.Field;

public abstract class SourceField extends Field {
    private String _fieldAsName;

    public String get_fieldAsName() {
        return _fieldAsName;
    }

    public void set_fieldAsName(String fieldAsName) {
        this._fieldAsName = fieldAsName;
    }
}
