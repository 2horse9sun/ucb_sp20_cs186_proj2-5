package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private BacktrackingIterator<Page> leftPageIterator;
        private BacktrackingIterator<Page> rightPageIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            // TODO(proj3_part1): implement
            this.rightIterator = SortMergeOperator.this.getRecordIterator(new SortOperator(getTransaction(),getRightTableName(),new RightRecordComparator()).sort());
            this.leftIterator = SortMergeOperator.this.getRecordIterator(new SortOperator(getTransaction(),getLeftTableName(),new LeftRecordComparator()).sort());

//            this.rightPageIterator = SortMergeOperator.this.getPageIterator(getRightTableName());
//            this.leftPageIterator = SortMergeOperator.this.getPageIterator((getLeftTableName()));

            this.nextRecord = null;

            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;

            this.marked = false;
            // We mark the first record so we can reset to it when we advance the left record.
            if (rightRecord != null) {
                rightIterator.markPrev();
            } else { return; }
            if (leftRecord != null) {
                leftIterator.markPrev();
            } else { return; }

            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }

        }

//        private void fetchNextRightPage(){
//            if(!rightPageIterator.hasNext()){
//                rightIterator = null;
//                rightRecord = null;
//                throw new NoSuchElementException("All Done!");
//            }
//            this.rightIterator = SortMergeOperator.this.getBlockIterator(this.getRightTableName(),rightPageIterator,1);
//        }

//        private void fetchNextLeftPage(){
//            if(!leftPageIterator.hasNext()){
//                leftIterator = null;
//                leftRecord = null;
//                throw new NoSuchElementException("All Done!");
//            }
//            this.leftIterator = SortMergeOperator.this.getBlockIterator(this.getRightTableName(),leftPageIterator,1);
//        }


        private void fetchNextLeftRecord() {
//            if (!leftIterator.hasNext()) fetchNextLeftPage();
            if(!leftIterator.hasNext()){
                leftRecord = null;
//                throw new NoSuchElementException("All Done!");
            }else{
                leftRecord = leftIterator.next();
            }
        }

        private void fetchNextRightRecord() {
//            if (!rightIterator.hasNext()) fetchNextRightPage();
            if(!rightIterator.hasNext()){
                if(leftIterator.hasNext()){
                    fetchNextLeftRecord();
                    DataBox leftJoinValue = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                    DataBox rightJoinValue = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
                    if(leftJoinValue.compareTo(rightJoinValue) <= 0){
                        resetRightRecord();
                    }
                }else{
                    rightRecord = null;
                }
            }else{
                rightRecord = rightIterator.next();
            }
        }

        private void resetRightRecord() {
            rightIterator.reset();

//            assert(rightRecordIterator.hasNext());
            rightRecord = rightIterator.next();
            rightIterator.markPrev();
        }



        private void fetchNextRecord() {
            nextRecord = null;
            while(leftRecord != null && rightRecord != null){
                DataBox leftJoinValue = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                DataBox rightJoinValue = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
                if(!marked){
                    while(leftJoinValue.compareTo(rightJoinValue) < 0){
                        fetchNextLeftRecord();
                        leftJoinValue = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                    }
                    while(leftJoinValue.compareTo(rightJoinValue) > 0){
                        fetchNextRightRecord();
                        rightJoinValue = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
                    }
                    rightIterator.markPrev();
                    marked = true;
                }
                if(leftJoinValue.equals(rightJoinValue)){
                    nextRecord = joinRecords(leftRecord,rightRecord);
                    fetchNextRightRecord();
                    break;
                }
                else{
                    resetRightRecord();
                    fetchNextLeftRecord();
                    marked = false;
                }

            }



        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(proj3_part1): implement
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(proj3_part1): implement
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
