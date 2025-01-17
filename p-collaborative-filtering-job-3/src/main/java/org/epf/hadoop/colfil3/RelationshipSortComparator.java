package org.epf.hadoop.colfil3;

import org.apache.commons.math3.optim.linear.Relationship;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RelationshipSortComparator extends WritableComparator {
    public RelationshipSortComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text ka = (Text)a;
        Text kb = (Text)b;

        return ka.toString().compareTo(kb.toString());
    }
}
