package org.hcjf.names;

/**
 * This naming consumer transform the camel case format to cassandra and vice versa
 * format all the resources name.
 * @author javaito
 * @mail javaito@gmail.com
 */
public class CassandraNaming extends NamingConsumer {

    public static final String CASSANDRA_NAMING_IMPL = "cassandra";

    private static final String NAME_SEPARATOR = "_";

    public CassandraNaming() {
        super(CASSANDRA_NAMING_IMPL);
    }

    /**
     * Normalize the name.
     * If the name is in came case this method transform all the upper case character in
     * a name separator character ('_') follow by the same character in lower case.
     * If the name is in the data base format, transform all the name separator characters ('_') in
     * a next character in upper case and delete the name separator character.
     * @param value Value to will be normalized.
     * @return Normalized value.
     */
    @Override
    public String normalize(String value) {
        StringBuilder result = new StringBuilder();
        char[] valueCharacters = value.toCharArray();
        char valueCharacter;
        if(!value.contains(NAME_SEPARATOR)) {
            for (int i = 0; i < valueCharacters.length; i++) {
                valueCharacter = valueCharacters[i];
                if (Character.isUpperCase(valueCharacter)) {
                    if (i != 0) {
                        result.append(NAME_SEPARATOR);
                    }
                    result.append(Character.toLowerCase(valueCharacter));
                } else {
                    result.append(valueCharacter);
                }
            }
        } else {
            for (int i = 0; i < valueCharacters.length; i++) {
                valueCharacter = valueCharacters[i];
                if (valueCharacter == NAME_SEPARATOR.charAt(0)) {
                    result.append(Character.toUpperCase(valueCharacters[++i]));
                } else {
                    result.append(valueCharacter);
                }
            }
        }
        return result.toString();
    }
}
