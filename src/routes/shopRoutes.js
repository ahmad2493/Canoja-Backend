const express = require("express");
const router = express.Router();
const shopController = require("../controllers/shopController");

/**
 * @swagger
 * /api/shops/compare:
 *   post:
 *     summary: Compare Google Maps shops with database records
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               lat:
 *                 type: number
 *                 example: 42.4449498
 *               lng:
 *                 type: number
 *                 example: -71.0288195
 *               radius:
 *                 type: number
 *                 example: 1000
 *     responses:
 *       200:
 *         description: Comparison successful
 *       500:
 *         description: Internal server error
 */
router.post("/compare", shopController.compareShops);

/**
 * @swagger
 * /api/shops/test-db:
 *   get:
 *     summary: Test database connection
 *     responses:
 *       200:
 *         description: Database stats retrieved successfully
 *       500:
 *         description: Database error
 */
router.get("/test-db", shopController.testDatabase);

module.exports = router;
