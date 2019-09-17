package com.moumou.bootmybatisdemo.dataAlignment.model;

public class SourceTableDateOffset {
	private String _sys;
	private String _db_sid;
	private String _table_schema;
	private String _table_name;
	private int _offset;
	
	public SourceTableDateOffset() {
		super();
	}

	public SourceTableDateOffset(String _sys, String _db_sid, String _table_schema, String _table_name,
			int _offset) {
		super();
		this._sys = _sys;
		this._db_sid = _db_sid;
		this._table_schema = _table_schema;
		this._table_name = _table_name;
		this._offset = _offset;
	}
	
	@Override
	public String toString() {
		return "SourceTableDateOffset [_sys=" + _sys + ", _db_sid=" + _db_sid + ", _table_schema=" + _table_schema
				+ ", _table_name=" + _table_name + ", _offset=" + _offset + "]";
	}

	public String get_table_schema() {
		return _table_schema;
	}
	public void set_table_schema(String _table_schema) {
		this._table_schema = _table_schema;
	}
	public String get_table_name() {
		return _table_name;
	}
	public void set_table_name(String _table_name) {
		this._table_name = _table_name;
	}
	public int get_offset() {
		return _offset;
	}
	public void set_offset(int _offset) {
		this._offset = _offset;
	}
	public String get_sys() {
		return _sys;
	}
	public void set_sys(String _sys) {
		this._sys = _sys;
	}
	public String get_db_sid() {
		return _db_sid;
	}
	public void set_db_sid(String _db_sid) {
		this._db_sid = _db_sid;
	}
	
}
