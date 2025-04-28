import os

from manim import *

print(type(config))

for key, value in config.items():
    print(key, value)
config.media_dir = os.path.join(os.path.dirname(__file__), "media")

config.save_as_gif = True


class DataFrameFlatten(Scene):
    def construct(self):

        # self.camera.frame.save_state()
        # -----------------------------
        # 1. Create the Original Table
        # -----------------------------
        # Imagine a dataframe with one nested column "Info"
        original_data = [
            ["Name", "Info", "Field3"],
            ["", "{'age': 30, 'city': 'NY'}", "{}"],
            ["Bob", "{'city': 'LA'}", "{}"],
        ]

        original_table = Table(
            original_data, include_outer_lines=True, h_buff=0.8, v_buff=0.5
        )

        self.play(FadeIn(original_table))

        # original_table_tmp = original_table.copy().scale(0.40).to_edge(LEFT, buff=1)
        self.play(original_table.animate.scale(0.40).to_edge(UP, buff=1))
        # self.play(original_table.animate.scale(0.40).to_edge(LEFT, buff=1))

        arrow = Arrow(start=UP, end=DOWN, buff=0.1)
        arrow.next_to(original_table, DOWN, buff=1)
        self.play(GrowArrow(arrow))

        new_data = [
            ["Field3.dummyVariable", "ID", "Info.age", "Info.city", "Name"],
            ["null", "1", "30", "NY", "null"],
            ["null", "2", "null", "LA", "Bob"],
        ]

        animations = []
        # new_table = original_table_tmp.to_edge(RIGHT, buff=1)
        new_table = original_table.copy()
        animations.append(new_table.animate.to_edge(DOWN, buff=1))
        animations.append(
            Transform(
                new_table,
                Table(new_data, include_outer_lines=True, h_buff=0.8, v_buff=0.5)
                .scale(0.45)
                .to_edge(DOWN, buff=1),
                replace_mobject_with_target_in_scene=False,
            )
        )
        # animations.append(
        #     Transform(
        #         original_table_tmp.copy(),
        #         new_table,
        #         replace_mobject_with_target_in_scene=False,
        #     )
        # )

        # self.play(
        #     Transform(
        #         original_table.copy(),
        #         new_table,
        #         replace_mobject_with_target_in_scene=True,
        #     )
        # )
        self.play(*animations)
        # self.play(original_table.copy().animate.to_edge(RIGHT, buff=1))

        # self.play(columns_new[0].animate.move_to(RIGHT * 5 + UP * 2.5))

        # self.play(columns_new[1].animate.move_to(RIGHT * 5 + UP * 2.5))

        # self.play(columns_new[2].animate.move_to(RIGHT * 5 + UP * 2.5))

        # columns_with_lines = []
        # print(len(horizontal_lines))
        # for i in enumerate(columns_new):
        #     tmp_table = new_table.copy()
        #     tmp_column = tmp_table.get_column(i)
        #     horizontal_lines = tmp_table.get_horizontal_lines()
        #     vertical_lines = tmp_table.get_vertical_lines()
        #     column_group = Group(
        #         tmp_column,
        #         horizontal_lines[i],
        #         horizontal_lines[i + 2],
        #         vertical_lines[i],
        #         vertical_lines[i + 2],
        #     )
        #     columns_with_lines.append(column_group)

        # column_new_1 = columns_with_lines[0]
        # self.play(column_new_1.animate.move_to(RIGHT * 5 + UP * 2.5))
        # # Animate creation of the original table.
        # self.play(Create(original_table))
        # self.wait(1)

        # # ------------------------------------------------------
        # # 2. Indicate the Flattening Transformation (Animation)
        # # ------------------------------------------------------
        # # For example, use an arrow to show "processing"
        # arrow = Arrow(start=LEFT, end=RIGHT, buff=0.1)
        # arrow.next_to(original_table, RIGHT, buff=1)
        # self.play(GrowArrow(arrow))
        # self.wait(1)

        # # Optionally, you could also highlight the nested cell.
        # # Here we assume the nested cell is at row 2, column 3.
        # # (Manim table cells are stored in a dictionary keyed by (row, col))
        # nested_cell = original_table.get_cell((2, 3))
        # highlight = SurroundingRectangle(nested_cell, color=YELLOW)
        # self.play(Create(highlight))
        # self.wait(1)
        # self.play(FadeOut(highlight))

        # # -----------------------------------------
        # # 3. Create the Flattened Dataframe Table
        # # -----------------------------------------
        # # In the flattened dataframe, the "Info" column is expanded into two columns.
        # flattened_data = [
        #     ["ID", "Name", "Info.age", "Info.city"],
        #     ["1", "Alice", "30", "NY"],
        #     ["2", "Bob", "25", "LA"],
        # ]
        # flattened_table = Table(
        #     flattened_data, include_outer_lines=True, h_buff=0.8, v_buff=0.5
        # ).scale(0.8)
        # flattened_table.next_to(arrow, RIGHT, buff=1)

        # # Animate the creation of the flattened table.
        # self.play(Create(flattened_table))
        # self.wait(2)

        # # Optionally, you can add further animations that "transform" the nested column
        # # into the two new columns. For instance, you could fade out the original "Info"
        # # column while simultaneously fading in the "Info.age" and "Info.city" columns.
        # # That might look like:
        # #
        # # info_cell = original_table.get_cell((1, 3))
        # # info_transform = Text("Info", font_size=24).move_to(info_cell.get_center())
        # # self.play(Transform(info_cell, info_transform))
        # #
        # # However, note that transforming individual cells might require more careful
        # # positioning depending on your exact design.

        # # End scene
        # self.wait(2)


if __name__ == "__main__":
    scene = DataFrameFlatten()
    scene.render()
