import * as React from "react"
import * as TabsPrimitive from "@radix-ui/react-tabs"
import { cn } from "@/lib/utils"

const Tabs = TabsPrimitive.Root

const TabsList = ({
  className,
  ...props
}: React.ComponentPropsWithoutRef<typeof TabsPrimitive.List>) => (
  <TabsPrimitive.List
    className={cn(
      "inline-flex h-11 items-center rounded-2xl border border-slate-800 bg-slate-950/40 p-1 text-slate-200 backdrop-blur",
      className
    )}
    {...props}
  />
)

const TabsTrigger = ({
  className,
  ...props
}: React.ComponentPropsWithoutRef<typeof TabsPrimitive.Trigger>) => (
  <TabsPrimitive.Trigger
    className={cn(
      "inline-flex items-center justify-center rounded-xl px-4 py-2 text-sm font-medium text-slate-300 transition",
      "hover:bg-slate-900 hover:text-white",
      "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/50",
      "data-[state=active]:bg-blue-600 data-[state=active]:text-white",
      "data-[state=active]:shadow-[0_0_24px_rgba(37,99,235,0.35)]",
      className
    )}
    {...props}
  />
)

const TabsContent = ({
  className,
  ...props
}: React.ComponentPropsWithoutRef<typeof TabsPrimitive.Content>) => (
  <TabsPrimitive.Content
    className={cn("mt-4", className)}
    {...props}
  />
)

export { Tabs, TabsList, TabsTrigger, TabsContent }
