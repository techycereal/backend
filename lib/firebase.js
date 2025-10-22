const admin = require("firebase-admin");

// Download your service account JSON from Firebase Console
// (Project Settings → Service accounts → Generate new private key)
const serviceAccount = require("./firebase-service-account.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

async function verifyToken(req, res, next) {
  const authHeader = req.headers.authorization;
    console.log(authHeader)
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return res.status(401).json({ error: "No token provided" });
  }

  const idToken = authHeader.split("Bearer ")[1];

  try {
    const decodedToken = await admin.auth().verifyIdToken(idToken);
    req.user = decodedToken; // attach user info to request
    next();
  } catch (error) {
    console.error("Token verification failed:", error);
    return res.status(403).json({ error: "Unauthorized" });
  }
}

module.exports = { verifyToken };
