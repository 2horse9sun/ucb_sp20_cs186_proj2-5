package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(proj4_part2): implement

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if(transaction == null) return;
        boolean isRoot = false;
        if(lockContext.parent == null) isRoot = true;
        if(isRoot){
            lockContext.acquire(transaction,lockType);
        }else{

            if(lockContext.numChildLocks.containsKey(transaction.getTransNum()) &&
            lockContext.numChildLocks.get(transaction.getTransNum()) >= 0){

                lockContext.escalate(transaction);
            }else{
                LockContext parentContext = lockContext.parent;
                List<LockContext> lockContextList = new ArrayList<LockContext>();
                lockContextList.add(lockContext);
                while(parentContext != null){
                    lockContextList.add(parentContext);
                    parentContext = parentContext.parent;
                }
                for(int i = lockContextList.size()-1;i >= 0;i--){
                    LockContext ctxt = lockContextList.get(i);
                    if(ctxt.getExplicitLockType(transaction) == LockType.NL){
                        if(getPriority(lockType) > getPriority(lockContext.parent.getExplicitLockType(transaction))){
                            if(ctxt.equals(lockContext)) ctxt.acquire(transaction,lockType);
                            else ctxt.acquire(transaction,LockType.parentLock(lockType));
                        }
                    }else{
                        if (getPriority(LockType.parentLock(lockType)) > getPriority(LockType.parentLock(ctxt.getExplicitLockType(transaction)))) {
                            if(ctxt.equals(lockContext)) ctxt.promote(transaction,lockType);
                            else{
                                if(lockType==LockType.X && ctxt.getExplicitLockType(transaction)==LockType.S)
                                    ctxt.promote(transaction,LockType.SIX);
                                else ctxt.promote(transaction,LockType.parentLock(lockType));
                            }
                        }
                    }
                }
            }
        }

        return;
    }

    // TODO(proj4_part2): add helper methods as you see fit
    public static int getPriority(LockType lockType) {
        int score;
        if (lockType == null) {
            score = 0;
        } else {
            switch (lockType) {
                case IS:
                    score = 1;
                    break;
                case SIX:
                    score = 2;
                    break;
                case IX:
                    score = 3;
                    break;
                case S:
                    score = 4;
                    break;
                case X:
                    score = 5;
                    break;
                default:
                    score = 0;
                    break;
            }
        }
        return score;
    }
}
