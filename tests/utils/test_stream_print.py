from dataneuron.utils.stream_print import stream_and_print_simplified_xml


def test_simplified_xml(capsys):
    chunks = [
        "<response>",
        "<sql>SELECT users.name, orders.id FROM users JOIN orders ON users.id = orders.user_id</sql>",
        "<explanation>This query joins the users and orders tables to retrieve user names and their corresponding order IDs.</explanation>",
        "<references>",
        "Referenced Elements:",
        "- Tables: users, orders",
        "- Columns: users.name, orders.id",
        "- Definitions: ..",
        "</references>",
        "<note>",
        "This query assumes that all users have placed at least one order. Users without orders will not appear in the result.",
        "</note>",
        "</response>"
    ]
    stream_and_print_simplified_xml(chunks)
    captured = capsys.readouterr()
    assert "Generated SQL Query:" in captured.out
    assert "SELECT users.name, orders.id FROM users JOIN orders ON users.id = orders.user_id" in captured.out
    assert "Explanation:" in captured.out
    assert "This query joins the users and orders tables" in captured.out
    assert "References:" in captured.out
    assert "Tables: users, orders" in captured.out
    assert "Note:" in captured.out
    assert "This query assumes that all users have placed at least one order" in captured.out
