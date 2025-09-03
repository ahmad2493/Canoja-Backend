const app = require("./src/app");
const PORT = process.env.PORT || 5000;

const server = app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
  console.log(`Health check at http://localhost:${PORT}/health`);
});

module.exports = server;
