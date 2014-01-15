## JugglingDB-RethinkDB

RethinkDB adapter for [jugglingdb](https://github.com/1602/jugglingdb).

## Usage

To use it you need `jugglingdb@0.2.x`.

1. Setup dependencies in `package.json`:

    ```json
    {
      ...
      "dependencies": {
        "jugglingdb": "0.2.x",
        "jugglingdb-rethink": "latest"
      },
      ...
    }
    ```

2. Use (default options, you can omit them):

    Important: database must exist.

    ```javascript
        var Schema = require('jugglingdb').Schema;
        var schema = new Schema('rethink', {
            host: "localhost",
            port: 28015,
            database: "test",
            poolMin: 1,
            poolMax: 10
        });
        ...
    ```

3. Connection pooling

    RethinkDB adapter supports connection pooling via generic-pool module.
    Pooling is enabled by default and can not be disabled for now.
    You can change pooling options specifying parameters poolMin (default: 1) and poolMax (default: 10).

## MIT License

    Copyright (C) 2013 by Dmitry Gorbunov

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
    THE SOFTWARE.

