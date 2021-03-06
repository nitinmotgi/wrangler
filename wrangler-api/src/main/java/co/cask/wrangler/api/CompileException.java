/*
 *  Copyright © 2017 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.wrangler.api;

import co.cask.wrangler.api.annotations.Public;
import co.cask.wrangler.api.parser.SyntaxError;

import java.util.Iterator;

/**
 * Class description here.
 */
@Public
public class CompileException extends Exception {
  private Iterator<SyntaxError> errors;

  public CompileException(String message) {
    super(message);
  }

  public CompileException(String message, Iterator<SyntaxError> errors) {
    super(message);
    this.errors = errors;
  }

  public CompileException(String message, Exception e) {
    super(message, e);
  }

  public Iterator<SyntaxError> iterator() {
    return errors;
  }
}
