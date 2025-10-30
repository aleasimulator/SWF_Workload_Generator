/*
 * LengthComparator.java
 *
 *
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.mycompany.swf_workload_generator;
import java.util.Comparator;
import com.mycompany.swf_workload_generator.Job;

/**
 * Class LengthComparator<p>
 * Compares two gridlets according to their estimated length
 * @author Dalibor Klusacek
 */
public class ArrivalComparator  implements Comparator {
    
    /**
     * Compares two gridlets according to their estimated length
     */
    public int compare(Object o1, Object o2) {
        Job g1 = (Job) o1;
        Job g2 = (Job) o2;
        long length1 = (Long) g1.getArrival();
        long length2 = (Long) g2.getArrival();
        if(length1 > length2) return 1;
        if(length1 == length2) return 0;
        if(length1 < length2) return -1;
        return 0;
    }
    
}
