package simpledb;

import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Math.max;


public class SuffixTree {
//    private static final Logger LOGGER = LoggerFactory.getLogger(SuffixTree.class);
    private static final String WORD_TERMINATION = "$";
    private static final int POSITION_UNDEFINED = -1;
    private Node root;
    private String fullText;
    private int string_cnt;

    public class Node {
        private String text;
        private List<Node> children;
        private int position;
        private double count;

        public Node(String word, int position, double count) {
            this.text = word;
            this.position = position;
            this.children = new ArrayList<>();
            this.count = count;
        }

        public String getText() { return text; }

        public void setText(String text) { this.text = text; }

        public int getPosition() { return position; }

        public double getCount() { return count;}

        public void setCount(double c) { count = c; }

        public void setPosition(int position) { this.position = position; }

        public List<Node> getChildren() { return children; }

        public void setChildren(List<Node> children) { this.children = children; }

        public String printTree(String depthIndicator) {
            String str = "";
            String positionStr = position > -1 ? "[" + String.valueOf(position) + "]" : "";
            String countStr = String.format("[%.1f]", getCount());
            str += depthIndicator + text + countStr + "\n";
            // str += depthIndicator + text + positionStr + "\n";

            for (int i = 0; i < children.size(); i++) {
                str += children.get(i)
                        .printTree(depthIndicator + "\t");
            }
            return str;
        }

        @Override
        public String toString() {
            return printTree("");
        }
    }

    public SuffixTree(String text) {
        root = new Node("", POSITION_UNDEFINED, -1);
        HashSet<Node> visited = new HashSet<Node>();
        for (int i = 0; i <= text.length(); i++) {
            // Debug.log("Adding suffix: %s", text.substring(i) + WORD_TERMINATION);
            addSuffix(text.substring(i) + WORD_TERMINATION, i, visited);
        }
        fullText = text;
        this.string_cnt = 1;

//        for (Node n: visited) {
//            n.setCount(n.getCount() + 1.0);
//        }
    }

    public void insertString(String text) {
        // root = new Node("", POSITION_UNDEFINED);
        HashSet<Node> visited = new HashSet<Node>();
        for (int i = 0; i <= text.length(); i++) {
            addSuffix(text.substring(i) + WORD_TERMINATION, i, visited);
        }
        // fullText = text;
        for (Node n: visited) {
            n.setCount(n.getCount() + 1.0);
        }
        this.string_cnt += 1;
    }

    public double searchText(String pattern) {
        // LOGGER.info("Searching for pattern \"{}\"", pattern);
//        List<String> result = new ArrayList<>();
        List<Node> nodes = getAllNodesInTraversePath(pattern, root, false);

        // TODO: maybe don't need this block, just traverse nodes and add the counts up.
//        if (nodes.size() > 0) {
//            Node lastNode = nodes.get(nodes.size() - 1);
//            if (lastNode != null) {
//                List<Integer> positions = getPositions(lastNode);
//                positions = positions.stream()
//                        .sorted()
//                        .collect(Collectors.toList());
//                positions.forEach(m -> result.add((markPatternInText(m, pattern))));
//            }
//        }

        //for (Node n: nodes) {
        if (!nodes.isEmpty()) {
            // Debug.log("Nodes: %s", nodes);
            Node n = nodes.get(nodes.size() - 1);
             Debug.log("Traversed node: %s[%.2f]/%d", n.getText(), n.getCount(), this.string_cnt);
            return n.getCount();
        } else {
            Debug.log("NO match");
            return 0;
        }

    }

    private void addSuffix(String suffix, int position, Set<Node> visited) {
//        LOGGER.info(">>>>>>>>>>>> Adding new suffix {}", suffix);
        List<Node> nodes = getAllNodesInTraversePath(suffix, root, true);
        if (nodes.size() == 0) {
            // Debug.log("suffix[%s] nodes: null", suffix);
            addChildNode(root, suffix, position);
//            LOGGER.info("{}", printTree());
        } else {
            String node_str = "";
            for (Node n: nodes) {
                visited.add(n);
                node_str += (n.getText() + ", ");
            }
            // Debug.log("suffix[%s] nodes: [%s]", suffix, node_str);
            Node lastNode = nodes.remove(nodes.size() - 1);
            String newText = suffix;
            if (nodes.size() > 0) {
                String existingSuffixUptoLastNode = nodes.stream()
                        .map(a -> a.getText())
                        .reduce("", String::concat);

                // Debug.log("existsuffixuptolast: %s", existingSuffixUptoLastNode);
                // Remove prefix from newText already included in parent
                newText = newText.substring(existingSuffixUptoLastNode.length());
            }
            extendNode(lastNode, newText, position);
        }
    }

    private List<Integer> getPositions(Node node) {
        List<Integer> positions = new ArrayList<>();
        if (node.getText().endsWith(WORD_TERMINATION)) {
            positions.add(node.getPosition());
        }
        for (int i = 0; i < node.getChildren().size(); i++) {
            positions.addAll(getPositions(node.getChildren().get(i)));
        }
        return positions;
    }

    private String markPatternInText(Integer startPosition, String pattern) {
        String matchingTextLHS = fullText.substring(0, startPosition);
        String matchingText = fullText.substring(startPosition, startPosition + pattern.length());
        String matchingTextRHS = fullText.substring(startPosition + pattern.length());
        return matchingTextLHS + "[" + matchingText + "]" + matchingTextRHS;
    }

    private void addChildNode(Node parentNode, String text, int position) {
        Node to_add = new Node(text, position, 1.0); // TODO: set count
        parentNode.getChildren().add(to_add);
        // parentNode.setCount(parentNode.getCount() + 1.0);
    }

    private void extendNode(Node node, String newText, int position) {
        // Debug.log("Extending Node[%s]; newText[%s], pos[%d]", node, newText, position);
        String currentText = node.getText();
        String commonPrefix = getLongestCommonPrefix(currentText, newText);

        if (commonPrefix != currentText) {
            String parentText = currentText.substring(0, commonPrefix.length());
            String childText = currentText.substring(commonPrefix.length());
            splitNodeToParentAndChild(node, parentText, childText);
        }

        String remainingText = newText.substring(commonPrefix.length());
        if (remainingText.length() > 0) {
            addChildNode(node, remainingText, position);
        } else {
            // node.setCount(node.getCount() + 1.0);
        }
    }

    private void splitNodeToParentAndChild(Node parentNode, String parentNewText, String childNewText) {
        Node childNode = new Node(childNewText, parentNode.getPosition(), 1.0);

        if (parentNode.getChildren().size() > 0) {
            while (parentNode.getChildren().size() > 0) {
                childNode.getChildren().add(parentNode.getChildren().remove(0));
            }
        }

        parentNode.getChildren().add(childNode);
        parentNode.setText(parentNewText);
        // parentNode.setCount(parentNode.getCount() + 1);
        parentNode.setPosition(POSITION_UNDEFINED);
    }

    private String getLongestCommonPrefix(String str1, String str2) {
        int compareLength = Math.min(str1.length(), str2.length());
        for (int i = 0; i < compareLength; i++) {
            if (str1.charAt(i) != str2.charAt(i)) {
                return str1.substring(0, i);
            }
        }
        return str1.substring(0, compareLength);
    }

    private List<Node> getAllNodesInTraversePath(String pattern, Node startNode, boolean isAllowPartialMatch) {
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < startNode.getChildren().size(); i++) {
            Node currentNode = startNode.getChildren().get(i);
            String nodeText = currentNode.getText();
            // Debug.log("pattern: %s, nodeText: %s", pattern, nodeText);
            if (pattern.length() == 0) {
                if (nodeText.equals("$")) {
                    nodes.add(currentNode);
                    return nodes;
                } else {
                    continue;
                }
            }
            if (pattern.charAt(0) == nodeText.charAt(0)) {
                if (isAllowPartialMatch && pattern.length() <= nodeText.length()) {
                    nodes.add(currentNode);
//                    currentNode.setCount(currentNode.getCount() + 1.0);
                    return nodes;
                }

                int compareLength = Math.min(nodeText.length(), pattern.length());
                for (int j = 1; j < compareLength; j++) {
                    if (pattern.charAt(j) != nodeText.charAt(j)) {
                        if (isAllowPartialMatch) {
                            nodes.add(currentNode);
//                            currentNode.setCount(currentNode.getCount() + 1.0);
                        }
                        return nodes;
                    }
                }

                nodes.add(currentNode);
//                currentNode.setCount(currentNode.getCount() + 1.0);
                if (pattern.length() > compareLength) {
                    List<Node> nodes2 = getAllNodesInTraversePath(pattern.substring(compareLength), currentNode, isAllowPartialMatch);
                    if (nodes2.size() > 0) {
                        nodes.addAll(nodes2);
                    } else if (!isAllowPartialMatch) {
                        // nodes.add(null);
                    }
                }
                return nodes;
            }
        }
        return nodes;
    }

    public String printTree() {
        return root.printTree("");
    }
}
