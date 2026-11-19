import java.security.SecureRandom;
import java.util.HexFormat;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * Prints a Trino password file: one {@code user:iterations:salt:hash} PBKDF2 line per user, the
 * password read from {@code TRINO_<USER>_PASSWORD}. Run with the image's JDK, which spares the
 * stack an image only to hash a few passwords.
 */
public final class HashPassword {
    // Trino verifies PBKDF2 with HMAC-SHA1: OWASP's iteration count for that function, and one
    // output block, since more blocks cost the server but not an attacker, who checks the first.
    private static final int ITERATIONS = 1_300_000;
    private static final int SALT_BYTES = 16;
    private static final int HASH_BITS = 160;

    public static void main(String[] users) throws Exception {
        var factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
        var random = new SecureRandom();
        var hex = HexFormat.of();
        for (String user : users) {
            String variable = "TRINO_" + user.toUpperCase() + "_PASSWORD";
            String password = System.getenv(variable);
            if (password == null || password.isEmpty()) {
                throw new IllegalStateException(variable + " is not set");
            }
            byte[] salt = new byte[SALT_BYTES];
            random.nextBytes(salt);
            var spec = new PBEKeySpec(password.toCharArray(), salt, ITERATIONS, HASH_BITS);
            byte[] hash = factory.generateSecret(spec).getEncoded();
            System.out.println(user + ":" + ITERATIONS + ":" + hex.formatHex(salt) + ":" + hex.formatHex(hash));
        }
    }
}
