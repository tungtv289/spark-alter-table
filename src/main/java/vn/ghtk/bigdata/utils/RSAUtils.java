package vn.ghtk.bigdata.utils;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

public class RSAUtils {
    private static final String ALGORITHM = "RSA";

    public static KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance(ALGORITHM);
        generator.initialize(512);
        KeyPair pair = generator.generateKeyPair();
        System.out.println("PUBLIC: " + Base64.getEncoder().encodeToString(pair.getPublic().getEncoded()));
        System.out.println("PRIVATE: " + Base64.getEncoder().encodeToString(pair.getPrivate().getEncoded()));
        return pair;
    }

    public static String encrypt(String secretText, String publicKey) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException, InvalidKeySpecException {
        if (secretText == null || publicKey == null) return null;
        byte[] byteKey = Base64.getDecoder().decode(publicKey.getBytes());
        X509EncodedKeySpec X509publicKey = new X509EncodedKeySpec(byteKey);
        KeyFactory kf = KeyFactory.getInstance(ALGORITHM);
        Cipher encryptCipher = Cipher.getInstance(ALGORITHM);
        encryptCipher.init(Cipher.ENCRYPT_MODE, kf.generatePublic(X509publicKey));
        byte[] secretMessageBytes = secretText.getBytes(StandardCharsets.UTF_8);
        byte[] encryptedMessageBytes = encryptCipher.doFinal(secretMessageBytes);
        String encodedMessage = Base64.getEncoder().encodeToString(encryptedMessageBytes);
        return encodedMessage;
    }

    public static String decrypt(String cipherText, String privateKey) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException, InvalidKeySpecException {
        if (cipherText == null || privateKey == null) return null;
        byte[] byteKey = Base64.getDecoder().decode(privateKey.getBytes());
        PKCS8EncodedKeySpec X509publicKey = new PKCS8EncodedKeySpec(byteKey);
        KeyFactory kf = KeyFactory.getInstance(ALGORITHM);
        Cipher decryptCipher = Cipher.getInstance(ALGORITHM);
        decryptCipher.init(Cipher.DECRYPT_MODE, kf.generatePrivate(X509publicKey));
        byte[] decryptedMessageBytes = decryptCipher.doFinal(Base64.getDecoder().decode(cipherText));
        return new String(decryptedMessageBytes, StandardCharsets.UTF_8);
    }

    public static void main(String[] args) throws NoSuchPaddingException, IllegalBlockSizeException, NoSuchAlgorithmException, BadPaddingException, InvalidKeySpecException, InvalidKeyException {
        String encrypt = encrypt("OqGmw9kuJilENbP898M8",
                "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAJHDyKQkarea52DO36e4xs5trwWaXN/GpupufE2MCykOLRK+O9SYm2y5RCUoq1n/T4//u2FvXFWyhBLCtt598UUCAwEAAQ==");
        System.out.println(encrypt);
    }
}
