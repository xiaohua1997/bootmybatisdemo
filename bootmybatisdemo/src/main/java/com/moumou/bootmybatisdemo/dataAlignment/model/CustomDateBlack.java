package com.moumou.bootmybatisdemo.dataAlignment.model;

import com.moumou.bootmybatisdemo.util.ExcelColumn;

public class CustomDateBlack extends PageNum{

	@ExcelColumn("batchDate")
	private String batchdate;
	@ExcelColumn("comment")
	private String comment;
	
	public CustomDateBlack() {
	}

	public CustomDateBlack(String batchdate, String comment) {
		this.batchdate = batchdate;
		this.comment = comment;
	}

	public String getBatchdate() {
		return batchdate;
	}

	public void setBatchdate(String batchdate) {
		this.batchdate = batchdate;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	@Override
	public String toString() {
		return "CustomDateBlack [batchdate=" + batchdate + ", comment=" + comment + "]";
	}
	
}
