package org.bigdata.spark.spark_the_definitive_guide.chap02_spark_overview;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * @Description:
 * @Author: FanYueXiang
 * @Date: 2020/4/29 11:54 PM
 */
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class FlightData implements Serializable {

    private static final long serialVersionUID = -1456253206415098236L;

    /**
     * 目标城市名称
     */
    private String DEST_COUNTRY_NAME;

    private String ORIGIN_COUNTRY_NAME;

    private Integer count;
}
