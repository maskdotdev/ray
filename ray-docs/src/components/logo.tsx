import type { Component } from 'solid-js'

interface LogoProps {
  class?: string
  size?: number
}

export const Logo: Component<LogoProps> = (props) => {
  const size = () => props.size ?? 32
  
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 200 240"
      fill="none"
      class={props.class}
      width={size()}
      height={size() * 1.2}
      aria-hidden="true"
    >
      {/* Strong Neon Background Glow */}
      <circle cx="108" cy="115" r="70" fill="url(#neonGlow)" fill-opacity="0.25" />

      {/* The Kite Fill (Subtle Blue Tint) */}
      <path d="M100 20L175 90L115 210L35 105L100 20Z" fill="url(#kiteFill)" fill-opacity="0.15"/>
      
      {/* Edges (Outer Boundary) */}
      <g stroke="url(#edgeGradient)" stroke-width="3" stroke-linecap="round" stroke-linejoin="round">
        <path d="M100 20L175 90" />
        <path d="M175 90L115 210" />
        <path d="M115 210L35 105" />
        <path d="M35 105L100 20" />
        
        {/* Edges (Internal Hub) */}
        <path d="M100 20L108 108" />
        <path d="M175 90L108 108" />
        <path d="M115 210L108 108" />
        <path d="M35 105L108 108" />
      </g>
      
      {/* Vertices (Nodes) - Glowing Dots */}
      <circle cx="100" cy="20" r="5" fill="#06B6D4" stroke="white" stroke-width="1.5" />
      <circle cx="175" cy="90" r="5" fill="#06B6D4" stroke="white" stroke-width="1.5" />
      <circle cx="115" cy="210" r="5" fill="#3B82F6" stroke="white" stroke-width="1.5" />
      <circle cx="35" cy="105" r="5" fill="#06B6D4" stroke="white" stroke-width="1.5" />
      
      {/* Center Node (The "Hub") */}
      <circle cx="108" cy="108" r="7" fill="white" />
      {/* Outer ring for center node */}
      <circle cx="108" cy="108" r="14" stroke="#00F0FF" stroke-width="1.5" stroke-opacity="0.6" stroke-dasharray="4 2" />

      <defs>
        {/* Electric Blue Gradient for Lines */}
        <linearGradient id="edgeGradient" x1="100" y1="20" x2="115" y2="210" gradientUnits="userSpaceOnUse">
          <stop stop-color="#00F0FF"/>
          <stop offset="1" stop-color="#2563EB"/>
        </linearGradient>
        
        {/* Subtle Fill Gradient */}
        <linearGradient id="kiteFill" x1="100" y1="20" x2="115" y2="210" gradientUnits="userSpaceOnUse">
          <stop stop-color="#22D3EE"/>
          <stop offset="1" stop-color="#1E40AF"/>
        </linearGradient>
        
        {/* Central Glow */}
        <radialGradient id="neonGlow">
          <stop offset="0%" stop-color="#00F0FF" />
          <stop offset="100%" stop-color="transparent" />
        </radialGradient>
      </defs>
    </svg>
  )
}

export default Logo
