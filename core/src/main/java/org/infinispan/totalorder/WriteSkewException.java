package org.infinispan.totalorder;

import org.infinispan.CacheException;

/**
 * @author pedro
 *         Date: 19-06-2011
 */
public class WriteSkewException extends CacheException {
    public WriteSkewException() {
    }

    public WriteSkewException(Throwable cause) {
        super(cause);
    }

    public WriteSkewException(String msg) {
        super(msg);
    }

    public WriteSkewException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
