package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field;

public abstract class Field {
    private String _fieldCnName;
    private int _index;
    private String _fileType;
    public String get_fieldCnName() {
        return _fieldCnName;
    }

    public void set_fieldCnName(String fieldCnName) {
        this._fieldCnName = fieldCnName;
    }

    public int get_index() {
        return _index;
    }

    public void set_index(int index) {
        this._index = index;
    }

	public String get_fileType() {
		return _fileType;
	}

	public void set_fileType(String _fileType) {
		this._fileType = _fileType;
	}
    
}
