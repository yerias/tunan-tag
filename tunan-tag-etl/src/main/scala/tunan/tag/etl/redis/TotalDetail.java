package tunan.tag.etl.redis;

import com.alibaba.fastjson.JSON;

import java.util.ArrayList;

public class TotalDetail {

    private Integer total;
    private Double growthRate;
    private String growthRatePercent;
    private Integer[] data;
    private String[] date;

    public TotalDetail() {
    }

    public TotalDetail(Integer total, Double growthRate, String growthRatePercent, Integer[] data, String[] date) {
        this.total = total;
        this.growthRate = growthRate;
        this.growthRatePercent = growthRatePercent;
        this.data = data;
        this.date = date;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public Double getGrowthRate() {
        return growthRate;
    }

    public void setGrowthRate(Double growthRate) {
        this.growthRate = growthRate;
    }

    public String getGrowthRatePercent() {
        return growthRatePercent;
    }

    public void setGrowthRatePercent(String growthRatePercent) {
        this.growthRatePercent = growthRatePercent;
    }

    public Integer[] getData() {
        return data;
    }

    public void setData(Integer[] data) {
        this.data = data;
    }

    public String[] getDate() {
        return date;
    }

    public void setDate(String[] date) {
        this.date = date;
    }
}
