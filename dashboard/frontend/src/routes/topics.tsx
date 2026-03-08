import { createFileRoute } from "@tanstack/react-router";
import { Topics } from "../components/Topics";

export const Route = createFileRoute("/topics")({
  component: () => <Topics />,
});
