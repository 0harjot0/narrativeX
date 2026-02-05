from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command
from agents.topic_explorer.graph import workflow, TraversalState
from uuid import uuid4
import asyncio
import random



async def execute():
    memory = InMemorySaver()
    config = {"configurable": {"thread_id": str(uuid4())}}
    
    graph = workflow.compile(checkpointer=memory)
    async for mode, chunk in graph.astream(
        {"perform_creative": False,
         "traversal": None}, config=config, stream_mode=['custom', 'values']):
        print(f'{mode}: {chunk}')
        
    if chunk.get('__interrupt__'):
        topics = chunk["__interrupt__"][0].value['topics']
        selected_topic = random.choice(topics)
        async for mode, chunk in graph.astream(
            Command(resume=selected_topic),
            config=config,
            stream_mode=['custom', 'values']
        ):
            print(f"{mode}: {chunk}")


if __name__ == "__main__":
    asyncio.run(execute())
    # //check again
