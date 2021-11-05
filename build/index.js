"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const postgresql_1 = __importDefault(require("./postgresql"));
function connect(c, next) {
    if (c.use && ["postgresql"].indexOf(c.use) !== -1) {
        (0, postgresql_1.default)(c[c.use], next);
    }
    else {
        throw "Config references DB-engine that does not exist: " + c.use;
    }
}
exports.default = connect;
//# sourceMappingURL=index.js.map