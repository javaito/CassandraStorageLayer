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
        for(char valueCharacter : valueCharacters) {
            if(Character.isUpperCase(valueCharacter)) {
                result.append(NAME_SEPARATOR);
                result.append(Character.toLowerCase(valueCharacter));
            } else {
                result.append(valueCharacter);
            }
        }
        return result.toString();
    }
}
