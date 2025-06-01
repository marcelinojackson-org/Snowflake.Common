if (!globalThis.DOMException) {
  class MinimalDOMException extends Error {
    constructor(message = '', name = 'DOMException') {
      super(message);
      this.name = name;
    }
  }

  globalThis.DOMException = MinimalDOMException;
}

module.exports = globalThis.DOMException;
