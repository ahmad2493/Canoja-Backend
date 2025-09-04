const app = require("./src/app");
const PORT = process.env.PORT || 5000;

const server = app.listen(PORT, () => {
  console.log(`Server running on ${PORT}`);
});

module.exports = server;
