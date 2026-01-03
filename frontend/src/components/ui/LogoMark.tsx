import { cn } from "@/lib/utils";

type LogoMarkProps = {
  className?: string;
};

export default function LogoMark({ className }: LogoMarkProps) {
  return (
    <div
      className={cn(
        "relative grid place-items-center rounded-2xl bg-blue-500/15 border border-blue-500/30 shadow-[0_0_35px_rgba(37,99,235,0.35)]",
        className
      )}
    >
      {/* glow */}
      <div className="absolute inset-0 rounded-2xl bg-blue-500/25 blur-xl" />

      {/* basketball core */}
      <div className="relative h-5 w-5 rounded-full bg-blue-400 shadow-[0_0_25px_rgba(96,165,250,0.9)]">
        {/* vertical seam */}
        <div className="absolute left-1/2 top-0 h-full w-[1px] -translate-x-1/2 bg-blue-100/40" />
        {/* horizontal seam */}
        <div className="absolute top-1/2 left-0 w-full h-[1px] -translate-y-1/2 bg-blue-100/40" />
      </div>
    </div>
  );
}
