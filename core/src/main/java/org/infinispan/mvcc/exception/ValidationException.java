package org.infinispan.mvcc.exception;

import org.infinispan.CacheException;

/**
 * @author pedro
 *         Date: 02-10-2011
 */
public class ValidationException extends CacheException {

    public ValidationException() {
    }

    public ValidationException(Throwable cause) {
        super(cause);
    }

    public ValidationException(String msg) {
        super(msg);
    }

    public ValidationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
