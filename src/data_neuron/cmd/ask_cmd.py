import click
from ..core.data_neuron import DataNeuron
from ..utils.print import print_info, print_success


def query(question, context):
    """Ask a question about the database."""
    dn = DataNeuron(db_config='database.yaml', context=context, log=True)
    dn.initialize()

    result = dn.query(question)

    print_info("Original Question: " + result['original_question'])
    print_info("Refined Question: " + result['refined_question'])

    print_info("\nRefinement Changes:")
    for change in result['refinement_changes']:
        print_info("- " + change)

    print_info("\nGenerated SQL:")
    print_info(result['sql'])

    print_success("\nQuery Result:")
    print(result['result'])

    print_info("\nExplanation:")
    print_info(result['explanation'])
