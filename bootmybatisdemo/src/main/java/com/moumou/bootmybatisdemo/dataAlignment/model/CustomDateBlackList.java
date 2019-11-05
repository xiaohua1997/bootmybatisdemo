package com.moumou.bootmybatisdemo.dataAlignment.model;

import com.moumou.bootmybatisdemo.util.ExcelColumn;

public class CustomDateBlackList extends PageNum{
	@ExcelColumn("_batch_date")
	private String _batch_date;
	@ExcelColumn("_comment")
	private String _comment;
	
	public CustomDateBlackList() {
		super();
	}

	public CustomDateBlackList(String _batch_date, String _comment) {
		super();
		this._batch_date = _batch_date;
		this._comment = _comment;
	}

	@Override
	public String toString() {
		return "CustomDateBlackList [_batch_date=" + _batch_date + ", _comment=" + _comment + "]";
	}
	
	public String get_batch_date() {
		return _batch_date;
	}
	public void set_batch_date(String _batch_date) {
		this._batch_date = _batch_date;
	}
	public String get_comment() {
		return _comment;
	}
	public void set_comment(String _comment) {
		this._comment = _comment;
	}
	
	
}
