from crewai import LLM, Agent, Crew, Process, Task
from crewai.knowledge.source.text_file_knowledge_source import \
    TextFileKnowledgeSource
from crewai.project import CrewBase, agent, crew, task
from crewai_tools import CodeInterpreterTool, FileWriterTool

# from salesanalysisagent.tools.schema_mapping_tool import SchemaMappingTool
from salesanalysisagent.tools.clean_validate_tool import CleanValidateTool
from salesanalysisagent.tools.code_gen_tool import CodeGenTool
from salesanalysisagent.tools.data_format_validator import \
    FormatChangeDetectorTool
from salesanalysisagent.tools.data_loader_tool import DataLoaderTool
from salesanalysisagent.tools.inspect_tool import InspectTool
from salesanalysisagent.tools.schema_mapping_tool import SchemaMappingTool
from salesanalysisagent.tools.schema_validator import SchemaValidatorTool


@CrewBase
class SalesAnalysisAgent:
    """Sales Analysis Agent crew"""

    agents_config = "config/agents.yaml"
    tasks_config = "config/tasks.yaml"
    content = "Users name is John. He is 30 years old and lives in San Francisco."
    text_source = TextFileKnowledgeSource(file_paths=["code.py", "requirements.txt"])
    code_interpreter = CodeInterpreterTool()
    write_tool = FileWriterTool()

    @agent
    def data_loader(self) -> Agent:
        return Agent(
            config=self.agents_config["data_loader"],
            tools=[DataLoaderTool(), FileWriterTool()],
            verbose=True,
        )

    @agent
    def inspector(self) -> Agent:
        return Agent(
            config=self.agents_config["inspector"], tools=[InspectTool()], verbose=True
        )

    @agent
    def schema_mapper(self) -> Agent:
        return Agent(
            config=self.agents_config["schema_mapper"],
            tools=[SchemaMappingTool()],
            verbose=True,
        )

    @agent
    def schema_validator(self) -> Agent:
        return Agent(
            config=self.agents_config["schema_validator"],
            tools=[
                SchemaValidatorTool(),
                # CodeInterpreterTool(),
                FileWriterTool(),
            ],
            verbose=True,
            all_code_execution=True,
            # knowledge_sources=[self.text_source],
            # llm=self.gemini_llm,
        )

    @agent
    def cleaner_validator(self) -> Agent:
        return Agent(
            config=self.agents_config["cleaner_validator"],
            tools=[CleanValidateTool()],
            verbose=True,
        )

    @agent
    def data_format_validator(self) -> Agent:
        return Agent(
            config=self.agents_config["data_format_validator"],
            tools=[FormatChangeDetectorTool()],
            verbose=True,
        )

    @agent
    def code_generator(self) -> Agent:
        return Agent(
            config=self.agents_config["code_generator"],
            tools=[CodeGenTool()],
            verbose=True,
        )

    # @task
    # def data_loader_task(self) -> Task:
    #     return Task(
    #         config=self.tasks_config["data_loader_task"],
    #     )
    #
    #
    # @task
    # def inspect_task(self) -> Task:
    #     return Task(
    #         config=self.tasks_config["inspect_task"],
    #     )

    #
    # @task
    # def schema_mapping_task(self) -> Task:
    #     return Task(
    #         config=self.tasks_config["schema_mapping_task"],
    #     )
    #
    # @task
    # def schema_validator_task(self) -> Task:
    #     return Task(config=self.tasks_config["schema_validator_task"])

    # @task
    # def data_format_check_task(self) -> Task:
    #     return Task(config=self.tasks_config["data_format_check_task"])
    #
    # @task
    # def clean_validate_task(self) -> Task:
    #     return Task(
    #         config=self.tasks_config["clean_validate_task"],
    #     )
    #
    # @task
    # def code_gen_task(self) -> Task:
    #     return Task(
    #         config=self.tasks_config["code_gen_task"], output_file="generated_code.py"
    #     )
    #
    @crew
    def crew(self) -> Crew:
        return Crew(
            agents=self.agents,
            tasks=self.tasks,
            process=Process.sequential,
            verbose=True,
            knowledge_sources=[self.text_source],
        )


# Create a knowledge source
