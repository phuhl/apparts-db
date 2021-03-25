module.exports = {
  testRunner: "jest-circus/runner",
  testTimeout: 5000,
  clearMocks: true,
  coveragePathIgnorePatterns: ["<rootDir>/node_modules/", "<rootDir>/tests"],
  watchPathIgnorePatterns: ["/\\.#.*", "/.*~", "/#.*#"],
};
