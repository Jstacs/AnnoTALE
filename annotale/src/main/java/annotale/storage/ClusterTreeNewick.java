package annotale.storage;

import annotale.TALE;
import de.jstacs.clustering.hierachical.ClusterTree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

/**
 * Minimal Newick wrapper for the Jstacs ClusterTree class that stores TALE ids as leaf labels.
 */
public final class ClusterTreeNewick {

    private ClusterTreeNewick() {}

    public static String toNewickWithTaleIds(ClusterTree<TALE> tree, Function<TALE, Integer> idFn) {
        if (tree == null) {
            return null;
        }
        return toNewick(tree, "", idFn) + ";";
    }

    public static ClusterTree<TALE> fromNewickWithTaleIds(String newick,
          Map<Integer, TALE> taleById,
          Map<Integer, Integer> taleIndexById) {
        if (newick == null || newick.trim().isEmpty()) {
            return null;
        }
        Parser p = new Parser(newick);
        ClusterTree<TALE> root = p.parseTree(taleById, new IndexAssigner(taleIndexById));
        p.skipWhitespace();
        return root;
    }

    public static ClusterTree<TALE> fromNewickWithTaleIds(String newick,
          Map<Integer, TALE> taleById) {
        return fromNewickWithTaleIds(newick, taleById, null);
    }

    private static String toNewick(ClusterTree<TALE> tree, String indent,
          Function<TALE, Integer> idFn) {
        ClusterTree<TALE>[] subs = tree.getSubTrees();
        boolean isLeaf = subs == null || subs.length == 0;
        if (!isLeaf) {
            StringBuilder sb = new StringBuilder();
            String distSuffix = distanceSuffix(tree.getDistance());
            for (int i = 0; i < subs.length; i++) {
                if (i > 0) {
                    sb.append(indent).append(',');
                }
                sb.append(indent).append(toNewick(subs[i], indent + "\t", idFn));
                if (distSuffix != null) {
                    sb.append(distSuffix);
                }
            }
            return indent + "(\n" + sb + indent + "\n)";
        }
        TALE tale = tree.getClusterElements()[0];
        Integer id = idFn.apply(tale);
        if (id == null) {
            throw new IllegalArgumentException("Missing tale id for " + tale.getId());
        }
        return indent + id;
    }

    private static String distanceSuffix(double dist) {
        if (Double.isFinite(dist) && dist != 0.0) {
            return ":" + formatDistance(dist);
        }
        return null;
    }

    private static String formatDistance(double dist) {
        DecimalFormat df = new DecimalFormat("0.######", DecimalFormatSymbols.getInstance(Locale.US));
        return df.format(dist);
    }

    private static final class IndexAssigner {
        private final Map<Integer, Integer> indexById;
        private int nextIndex;

        private IndexAssigner(Map<Integer, Integer> indexById) {
            this.indexById = indexById;
            this.nextIndex = 0;
        }

        private int indexFor(Integer id) {
            if (indexById != null && indexById.containsKey(id)) {
                return indexById.get(id);
            }
            return nextIndex++;
        }
    }

    private static final class Parser {
        private final String s;
        private int pos;

        private Parser(String s) {
            this.s = s;
            this.pos = 0;
        }

        private ClusterTree<TALE> parseTree(Map<Integer, TALE> taleById, IndexAssigner assigner) {
            skipWhitespace();
            if (peek() == '(') {
                return parseInternal(taleById, assigner);
            }
            return parseLeaf(taleById, assigner);
        }

        private ClusterTree<TALE> parseInternal(Map<Integer, TALE> taleById, IndexAssigner assigner) {
            pos++; // '('
            skipWhitespace();
            List<ClusterTree<TALE>> children = new ArrayList<>();
            Double dist = null;
            while (true) {
                ClusterTree<TALE> child = parseTree(taleById, assigner);
                children.add(child);
                skipWhitespace();
                if (peek() == ':') {
                    pos++;
                    double parsed = parseNumber();
                    if (dist == null) {
                        dist = parsed;
                    }
                }
                skipWhitespace();
                char c = peek();
                if (c == ',') {
                    pos++;
                    skipWhitespace();
                    continue;
                }
                if (c == ')') {
                    pos++;
                    break;
                }
                throw new IllegalArgumentException("Malformed Newick near position " + pos);
            }
            ClusterTree<TALE>[] arr = children.toArray(new ClusterTree[0]);
            double value = dist == null ? 0.0 : dist.doubleValue();
            return new ClusterTree<>(value, -1, arr);
        }

        private ClusterTree<TALE> parseLeaf(Map<Integer, TALE> taleById, IndexAssigner assigner) {
            int id = parseInt();
            TALE tale = taleById.get(id);
            if (tale == null) {
                throw new IllegalArgumentException("Unknown tale id in Newick: " + id);
            }
            int idx = assigner.indexFor(id);
            return new ClusterTree<>(tale, idx);
        }

        private int parseInt() {
            skipWhitespace();
            int start = pos;
            while (pos < s.length() && Character.isDigit(s.charAt(pos))) {
                pos++;
            }
            if (start == pos) {
                throw new IllegalArgumentException("Expected integer at position " + pos);
            }
            return Integer.parseInt(s.substring(start, pos));
        }

        private double parseNumber() {
            skipWhitespace();
            int start = pos;
            while (pos < s.length()) {
                char c = s.charAt(pos);
                if (c == ',' || c == '(' || c == ')' || c == ';' || Character.isWhitespace(c)) {
                    break;
                }
                pos++;
            }
            if (start == pos) {
                throw new IllegalArgumentException("Expected number at position " + pos);
            }
            return Double.parseDouble(s.substring(start, pos));
        }

        private char peek() {
            if (pos >= s.length()) {
                return '\0';
            }
            return s.charAt(pos);
        }

        private void skipWhitespace() {
            while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) {
                pos++;
            }
        }
    }
}
