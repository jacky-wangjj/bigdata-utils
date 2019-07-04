package com.jacky.mr.groupsort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/4
 */
public class SortBean implements WritableComparable<SortBean> {
    private String id;
    private String name;
    private String mark;
    private String source;

    public SortBean() {
    }

    public SortBean(String id, String name, String mark, String source) {
        this.id = id;
        this.name = name;
        this.mark = mark;
        this.source = source;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMark() {
        return mark;
    }

    public void setMark(String mark) {
        this.mark = mark;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @Override
    public String toString() {
        return id + "\t" + name + "\t" + mark + "\t" + source;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(id);
        output.writeUTF(name);
        output.writeUTF(mark);
        output.writeUTF(source);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        id = input.readUTF();
        name = input.readUTF();
        mark = input.readUTF();
        source = input.readUTF();
    }

    /**
     * 多字段排序，先对mark分组，然后对同一分组中按id进行排序
     *
     * @param o
     * @return int
     *
     * @author wangjj17@lenovo.com
     * @date 2019/7/4
     */
    @Override
    public int compareTo(SortBean o) {
        int result;
        if (this.mark.compareTo(o.getMark()) > 0) {
            result = 1;
        } else if (this.mark.compareTo(o.getMark()) < 0) {
            result = -1;
        } else {
            result = this.id.compareTo(o.getId()) > 0 ? 1 : -1;
        }
        return result;
    }
}