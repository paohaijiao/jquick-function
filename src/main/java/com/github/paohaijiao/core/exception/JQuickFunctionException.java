/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright (c) [2025-2099] Martin (goudingcheng@gmail.com)
 */
package com.github.paohaijiao.core.exception;

/**
 * packageName com.github.paohaijiao.core.exception
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/4
 */
public class JQuickFunctionException extends RuntimeException {

    /**
     * 错误码（可选，用于统一异常码体系）
     */
    private int errorCode;


    public JQuickFunctionException() {
        super();
    }

    public JQuickFunctionException(String message) {
        super(message);
    }

    public JQuickFunctionException(String message, Throwable cause) {
        super(message, cause);
    }

    public JQuickFunctionException(Throwable cause) {
        super(cause);
    }

    public JQuickFunctionException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public JQuickFunctionException(int errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }
}
