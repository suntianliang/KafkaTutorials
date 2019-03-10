package com.yq.customized;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Simple to Introduction
 * className: Statistics
 *
 * @author EricYang
 * @version 2019/3/9 11:58
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Statistics {
    private Long avg;
    private Long sum;
    private Long count;
}
