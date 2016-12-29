package org.hcjf.names;

/**
 * @author javaito
 * @mail javaito@gmail.com
 */
public class CassandraNaming extends NamingConsumer {

    public static final String CASSANDRA_NAMING_IMPL = "cassandra";

    private static final char NAME_SEPARATOR = '_';

    public CassandraNaming() {
        super(CASSANDRA_NAMING_IMPL);
    }

    /**
     *
     * @param value
     * @return
     */
    @Override
    public String normalize(String value) {
        StringBuilder result = new StringBuilder();
        char[] valueCharacters = value.toCharArray();
        for (int i = 0; i < valueCharacters.length; i++) {
            char valueCharacter = valueCharacters[i];
            if(Character.isUpperCase(valueCharacter)) {
                if(i != 0) {
                    result.append(NAME_SEPARATOR);
                }
                result.append(Character.toLowerCase(valueCharacter));
            } else {
                result.append(valueCharacter);
            }
        }
        return result.toString();
    }
}
